(function () {
  'use strict';
  const panel = document.getElementById('pendingArchives');
  if (!panel) return;
  const list = document.getElementById('pendingArchiveItems');
  const error = document.getElementById('pendingArchiveError');
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
  function publish(items) {
    window.vireoPendingArchives = items;
    try {
      window.dispatchEvent(new CustomEvent('vireo:pending-archives-changed', {detail: {items: items}}));
    } catch (_error) {}
  }

  async function refresh() {
    if (loading || sending || document.hidden) return;
    loading = true;
    try {
      const response = await fetch('/api/import/pending-archives');
      if (!response.ok) throw new Error('Could not check photos waiting for NAS transfer.');
      const data = await response.json();
      error.textContent = actionError;
      const next = JSON.stringify(data.items || []);
      if (next === signature) return;
      signature = next;
      publish(data.items || []);
      const expanded = new Set(Array.from(list.querySelectorAll('details[open]'), el => el.dataset.archiveId));
      list.replaceChildren();
      panel.hidden = !data.items.length;
      data.items.forEach(item => {
        const row = document.createElement('div');
        row.className = 'pending-archive-row';
        const details = document.createElement('details');
        details.className = 'pending-archive-details';
        details.dataset.archiveId = String(item.id);
        details.open = expanded.has(String(item.id));
        const title = document.createElement('summary');
        title.textContent = item.name;
        details.appendChild(title);
        const destination = document.createElement('div');
        destination.className = 'pending-archive-destination';
        destination.textContent = 'Destination: ' + item.destination;
        details.appendChild(destination);
        row.appendChild(details);
        const actions = document.createElement('div');
        actions.className = 'pending-archive-actions';
        row.appendChild(actions);
        if (item.collection_id) {
          const review = document.createElement('a');
          review.href = '/browse?collection_id=' + encodeURIComponent(item.collection_id);
          review.textContent = 'Review photos';
          actions.appendChild(review);
        }
        const sendButtons = [];
        async function startSend(syncFirst) {
          if (sending) return;
          sending = true;
          sendButtons.forEach(b => { b.disabled = true; });
          error.textContent = '';
          actionError = '';
          try {
            const response = await fetch('/api/import/pending-archives/' + encodeURIComponent(item.id) + '/send', {
              method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({sync_first: syncFirst}),
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.error || 'Could not start the NAS transfer.');
          } catch (e) {
            actionError = e.message;
            error.textContent = actionError;
          } finally {
            sending = false;
            signature = '';
            refresh();
          }
        }
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn';
        button.textContent = item.state === 'sending' ? 'Sending to NAS…' : 'Send to NAS';
        button.disabled = item.state !== 'ready';
        button.addEventListener('click', () => startSend(false));
        sendButtons.push(button);
        if (item.unsynced_photos) {
          // Offered ahead of the plain send because the cheap moment to write
          // these sidecars is now, on local disk: a verified transfer deletes
          // the originals, and every later sync goes over the NAS connection.
          const syncSend = document.createElement('button');
          syncSend.type = 'button';
          syncSend.className = 'btn';
          syncSend.textContent = 'Sync metadata and send to NAS';
          syncSend.disabled = item.state !== 'ready';
          syncSend.addEventListener('click', () => startSend(true));
          sendButtons.push(syncSend);
          actions.appendChild(syncSend);
        }
        actions.appendChild(button);
        if (item.unsynced_photos || item.unsynced_photos_other_workspaces) {
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
          unsynced.textContent = parts.join(' ');
          row.appendChild(unsynced);
        }
        if (item.source_available === false) {
          const missing = document.createElement('div');
          missing.className = 'pending-archive-note';
          missing.textContent = 'Local originals are unavailable. Reconnect their storage, or remove this transfer record if they are permanently gone.';
          row.appendChild(missing);
          const discard = document.createElement('button');
          discard.type = 'button';
          discard.className = 'btn';
          discard.textContent = 'Remove missing transfer';
          discard.disabled = item.state !== 'ready';
          discard.addEventListener('click', async () => {
            if (sending || !window.confirm('Remove this missing transfer record? Vireo will stop tracking its NAS transfer. No files or catalog entries will be deleted. If the storage is only disconnected, cancel and reconnect it instead.')) return;
            sending = true;
            discard.disabled = true;
            actionError = '';
            error.textContent = '';
            try {
              const response = await fetch('/api/import/pending-archives/' + encodeURIComponent(item.id) + '/discard', {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({confirmed: true}),
              });
              const result = await response.json();
              if (!response.ok) throw new Error(result.error || 'Could not remove the transfer record.');
            } catch (e) {
              actionError = e.message;
              error.textContent = actionError;
            } finally {
              sending = false;
              signature = '';
              refresh();
            }
          });
          row.appendChild(discard);
        }
        if (item.error || item.state === 'waiting') {
          const note = document.createElement('div');
          note.className = 'pending-archive-note';
          note.textContent = item.error || 'Waiting for running jobs to finish.';
          row.appendChild(note);
        }
        list.appendChild(row);
      });
    } catch (e) {
      if (!panel.hidden) error.textContent = e.message;
    } finally {
      loading = false;
    }
  }
  refresh();
  setInterval(refresh, 5000);
  window.addEventListener('focus', refresh);
  document.addEventListener('visibilitychange', refresh);
})();
