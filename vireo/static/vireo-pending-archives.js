(function () {
  'use strict';
  const panelElement = document.getElementById('pendingArchives');
  if (!panelElement) return;
  const panel = panelElement;
  const listElement = document.getElementById('pendingArchiveItems');
  const errorElement = document.getElementById('pendingArchiveError');
  if (!listElement || !errorElement) return;
  const list = listElement;
  const error = errorElement;
  let signature = '';
  let loading = false;
  let sending = false;
  let actionError = '';

  // The banner is not the only place a pending transfer has to show up: the
  // Browse sidebar badges the staging folders these photos still sit in, and
  // it renders from whatever this poll last saw. Publishing the items (rather
  // than having the sidebar poll the endpoint itself) keeps both surfaces on
  // one snapshot, so a transfer can never look sent in one and pending in the
  // other.
  /** @param {PendingArchive[]} items */
  function publish(items) {
    window.vireoPendingArchives = items;
    try {
      window.dispatchEvent(new CustomEvent('vireo:pending-archives-changed', {detail: {items: items}}));
    } catch (_error) {}
  }

  /** @param {PendingArchive} item @param {HTMLElement} row */
  function appendUnsyncedNote(item, row) {
  if (item.unsynced_photos || item.unsynced_photos_other_workspaces || item.unsynced_photos_here_with_sibling_edits) {
    const unsynced = document.createElement('div');
    unsynced.className = 'pending-archive-note';
    const parts = [];
    if (item.unsynced_photos) {
      parts.push(item.unsynced_photos === 1
        ? '1 photo here has metadata changes that are not written to its sidecar yet. '
          + 'Writing it now is a local disk write; after the transfer the same sync has to run over the NAS connection.'
        : item.unsynced_photos + ' photos here have metadata changes that are not written to their sidecars yet. '
          + 'Writing them now is a local disk write; after the transfer the same sync has to run over the NAS connection.');
    }
    // Said separately because this button will not write them: the
    // sync queue is per workspace, so a number that lumped them in
    // would promise work that is not going to happen.
    if (item.unsynced_photos_other_workspaces) {
      const n = item.unsynced_photos_other_workspaces;
      parts.push(n === 1
        ? '1 more photo has changes queued in another workspace. "Sync metadata and send to NAS" will not write those — switch to that workspace and sync there first.'
        : n + ' more photos have changes queued in other workspaces. "Sync metadata and send to NAS" will not write those — switch to those workspaces and sync there first.');
    }
    // The overlap: photos already counted above whose sibling-workspace
    // edits will still miss the transfer even after this button runs.
    // Called out because the "here" number would otherwise read as
    // "these photos are covered" when only the active workspace's half
    // of their edits is.
    if (item.unsynced_photos_here_with_sibling_edits) {
      const n = item.unsynced_photos_here_with_sibling_edits;
      parts.push(n === 1
        ? '1 of those photos also has edits queued in another workspace. Only this workspace’s half will be written; switch to that workspace and sync there before the transfer to catch the rest.'
        : n + ' of those photos also have edits queued in other workspaces. Only this workspace’s half will be written; switch to those workspaces and sync there before the transfer to catch the rest.');
    }
    unsynced.textContent = parts.join(' ');
    row.appendChild(unsynced);
  }
  }

  /** @param {number | null | undefined} size */
  function archiveBytes(size) {
    if (size == null) return 'Size unavailable';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit += 1; }
    return size.toLocaleString(undefined, {maximumFractionDigits: 1}) + ' ' + units[unit] + ' estimated';
  }

  /** @param {string | undefined} created */
  function archiveAge(created) {
    // pending_archives.created_at is SQLite datetime('now'), an instant in UTC.
    if (!created) return 'Age unavailable';
    const iso = created.replace(' ', 'T');
    const date = new Date(/[zZ]|[+-]\d\d:\d\d$/.test(iso) ? iso : iso + 'Z');
    if (isNaN(date.getTime())) return 'Age unavailable';
    const days = Math.max(0, Math.floor((Date.now() - date.getTime()) / 86400000));
    return days ? 'Kept locally for ' + days + (days === 1 ? ' day' : ' days') : 'Kept locally for less than a day';
  }

  /** @param {PendingArchive} item @param {HTMLElement} details */
  function appendArchiveSummary(item, details) {
    const meta = document.createElement('div');
    meta.className = 'pending-archive-meta';
    const status = item.source_available === false ? 'Local originals unavailable'
      : item.state === 'sending' ? 'Sending to NAS'
        : item.state === 'waiting' ? 'Waiting for running jobs'
          : item.error ? 'Transfer needs attention' : 'Awaiting NAS transfer';
    const count = item.photo_count == null ? 'Photo count unavailable'
      : item.photo_count.toLocaleString() + (item.photo_count === 1 ? ' cataloged photo' : ' cataloged photos');
    meta.textContent = [count, archiveBytes(item.cataloged_bytes), archiveAge(item.created_at), status].join(' · ');
    details.appendChild(meta);
  }

  /** @param {PendingArchive} item @param {boolean} syncFirst @param {HTMLButtonElement[]} buttons */
  async function startSend(item, syncFirst, buttons) {
    if (sending) return;
    sending = true;
    buttons.forEach(button => { button.disabled = true; });
    error.textContent = '';
    actionError = '';
    try {
      const response = await fetch('/api/import/pending-archives/' + encodeURIComponent(String(item.id)) + '/send', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({sync_first: syncFirst}),
      });
      /** @type {{error?: string}} */
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Could not start the NAS transfer.');
    } catch (e) {
      actionError = e instanceof Error ? e.message : String(e);
      error.textContent = actionError;
    } finally {
      sending = false;
      signature = '';
      refresh();
    }
  }

  /** @param {PendingArchive} item @param {HTMLElement} row */
  function appendMissingSource(item, row) {
    if (item.source_available !== false) return;
    const missing = document.createElement('div');
    missing.className = 'pending-archive-note';
    missing.textContent = 'Local originals are unavailable. Reconnect their storage, or remove this transfer record if they are permanently gone.';
    row.appendChild(missing);
    const discard = document.createElement('button');
    discard.type = 'button';
    discard.className = 'btn btn-secondary';
    discard.textContent = 'Remove missing transfer';
    discard.disabled = item.state !== 'ready';
    discard.addEventListener('click', async () => {
      if (sending || !window.confirm('Remove this missing transfer record? Vireo will stop tracking its NAS transfer. No files or catalog entries will be deleted. If the storage is only disconnected, cancel and reconnect it instead.')) return;
      sending = true;
      discard.disabled = true;
      actionError = '';
      error.textContent = '';
      try {
        const response = await fetch('/api/import/pending-archives/' + encodeURIComponent(String(item.id)) + '/discard', {
          method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({confirmed: true}),
        });
        /** @type {{error?: string}} */
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || 'Could not remove the transfer record.');
      } catch (e) {
        actionError = e instanceof Error ? e.message : String(e);
        error.textContent = actionError;
      } finally {
        sending = false;
        signature = '';
        refresh();
      }
    });
    row.appendChild(discard);
  }

  /** @param {PendingArchive} item @param {Set<string | null>} expanded @param {Set<string | null>} existing */
  function renderArchive(item, expanded, existing) {
    const row = document.createElement('div');
    row.className = 'pending-archive-row';
    const details = document.createElement('details');
    details.className = 'pending-archive-details';
    details.dataset.archiveId = String(item.id);
    details.open = expanded.has(String(item.id)) || (!existing.has(String(item.id)) && panel.dataset.expanded === 'true');
    const title = document.createElement('summary');
    title.textContent = item.name;
    details.appendChild(title);
    const destination = document.createElement('div');
    destination.className = 'pending-archive-destination';
    destination.textContent = 'Destination: ' + item.destination;
    details.appendChild(destination);
    row.appendChild(details);
    appendArchiveSummary(item, row);
    const actions = document.createElement('div');
    actions.className = 'pending-archive-actions';
    row.appendChild(actions);
    if (item.collection_id) {
      const review = document.createElement('a');
      review.href = '/browse?collection_id=' + encodeURIComponent(String(item.collection_id));
      review.textContent = 'Review photos';
      actions.appendChild(review);
    }
    /** @type {HTMLButtonElement[]} */
    const buttons = [];
    if (item.unsynced_photos) {
      const syncSend = document.createElement('button');
      syncSend.type = 'button';
      syncSend.className = 'btn btn-secondary';
      syncSend.textContent = 'Sync metadata and send to NAS';
      syncSend.disabled = item.state !== 'ready' || item.source_available === false;
      syncSend.addEventListener('click', () => startSend(item, true, buttons));
      buttons.push(syncSend);
      actions.appendChild(syncSend);
    }
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn btn-secondary';
    button.textContent = item.state === 'sending' ? 'Sending to NAS…' : 'Send to NAS';
    button.disabled = item.state !== 'ready' || item.source_available === false;
    button.addEventListener('click', () => startSend(item, false, buttons));
    buttons.push(button);
    actions.appendChild(button);
    appendUnsyncedNote(item, row);
    appendMissingSource(item, row);
    if (item.error || item.state === 'waiting') {
      const note = document.createElement('div');
      note.className = 'pending-archive-note';
      note.textContent = item.error || 'Waiting for running jobs to finish.';
      row.appendChild(note);
    }
    list.appendChild(row);
  }

  const refresh = async function() {
    if (loading || sending || document.hidden) return;
    loading = true;
    try {
      const response = await fetch('/api/import/pending-archives');
      if (!response.ok) throw new Error('Could not check photos waiting for NAS transfer.');
      /** @type {{items: PendingArchive[]}} */
      const data = await response.json();
      if (!Array.isArray(data.items)) throw new Error('Could not check photos waiting for NAS transfer.');
      error.textContent = actionError;
      panel.hidden = !data.items.length;
      // Age advances while a dashboard stays open, even if the records do not change.
      const next = JSON.stringify(data.items) + '|' + Math.floor(Date.now() / 60000);
      if (next === signature) return;
      signature = next;
      publish(data.items);
      const expanded = new Set(Array.from(list.querySelectorAll('details[open]'), el => el.getAttribute('data-archive-id')));
      const existing = new Set(Array.from(list.querySelectorAll('details'), el => el.getAttribute('data-archive-id')));
      list.replaceChildren();
      data.items.forEach(item => renderArchive(item, expanded, existing));
    } catch (e) {
      panel.hidden = false;
      error.textContent = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  };
  refresh();
  setInterval(refresh, 5000);
  window.addEventListener('focus', refresh);
  document.addEventListener('visibilitychange', refresh);
})();
